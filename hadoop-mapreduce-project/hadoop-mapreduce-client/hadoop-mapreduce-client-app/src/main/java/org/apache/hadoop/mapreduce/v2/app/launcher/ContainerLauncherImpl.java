/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapreduce.v2.app.launcher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerManagerSecurityInfo;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * This class is responsible for launching of containers.
 */
public class ContainerLauncherImpl extends AbstractService implements
    ContainerLauncher {

  private static final Log LOG = LogFactory.getLog(ContainerLauncherImpl.class);

  private AppContext context;
  private ThreadPoolExecutor launcherPool;
  private static final int INITIAL_POOL_SIZE = 10;
  private int limitOnPoolSize;
  private Thread eventHandlingThread;
  private BlockingQueue<ContainerLauncherEvent> eventQueue =
      new LinkedBlockingQueue<ContainerLauncherEvent>();
  private RecordFactory recordFactory;
  //have a cache/map of UGIs so as to avoid creating too many RPC
  //client connection objects to the same NodeManager
  private ConcurrentMap<String, UserGroupInformation> ugiMap = 
    new ConcurrentHashMap<String, UserGroupInformation>();

  public ContainerLauncherImpl(AppContext context) {
    super(ContainerLauncherImpl.class.getName());
    this.context = context;
  }

  @Override
  public synchronized void init(Configuration conf) {
    // Clone configuration for this component so that the SecurityInfo setting
    // doesn't affect the original configuration
    Configuration myLocalConfig = new Configuration(conf);
    myLocalConfig.setClass(
        YarnConfiguration.YARN_SECURITY_INFO,
        ContainerManagerSecurityInfo.class, SecurityInfo.class);
    this.recordFactory = RecordFactoryProvider.getRecordFactory(conf);
    this.limitOnPoolSize = conf.getInt(
        MRJobConfig.MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT);
    super.init(myLocalConfig);
  }

  public void start() {
    // Start with a default core-pool size of 10 and change it dynamically.
    launcherPool = new ThreadPoolExecutor(INITIAL_POOL_SIZE,
        Integer.MAX_VALUE, 1, TimeUnit.HOURS,
        new LinkedBlockingQueue<Runnable>());
    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        ContainerLauncherEvent event = null;
        while (!Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return;
          }

          int poolSize = launcherPool.getCorePoolSize();

          // See if we need up the pool size only if haven't reached the
          // maximum limit yet.
          if (poolSize != limitOnPoolSize) {

            // nodes where containers will run at *this* point of time. This is
            // *not* the cluster size and doesn't need to be.
            int numNodes = ugiMap.size();
            int idealPoolSize = Math.min(limitOnPoolSize, numNodes);

            if (poolSize <= idealPoolSize) {
              // Bump up the pool size to idealPoolSize+INITIAL_POOL_SIZE, the
              // later is just a buffer so we are not always increasing the
              // pool-size
              launcherPool.setCorePoolSize(idealPoolSize + INITIAL_POOL_SIZE);
            }
          }

          // the events from the queue are handled in parallel
          // using a thread pool
          launcherPool.execute(new EventProcessor(event));

          // TODO: Group launching of multiple containers to a single
          // NodeManager into a single connection
        }
      }
    });
    eventHandlingThread.start();
    super.start();
  }

  public void stop() {
    eventHandlingThread.interrupt();
    launcherPool.shutdown();
    super.stop();
  }

  protected ContainerManager getCMProxy(ContainerId containerID,
      final String containerManagerBindAddr, ContainerToken containerToken)
      throws IOException {

    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    if (UserGroupInformation.isSecurityEnabled()) {

      Token<ContainerTokenIdentifier> token = new Token<ContainerTokenIdentifier>(
          containerToken.getIdentifier().array(), containerToken
              .getPassword().array(), new Text(containerToken.getKind()),
          new Text(containerToken.getService()));
      // the user in createRemoteUser in this context is not important
      UserGroupInformation ugi = UserGroupInformation
          .createRemoteUser(containerManagerBindAddr);
      ugi.addToken(token);
      ugiMap.putIfAbsent(containerManagerBindAddr, ugi);

      user = ugiMap.get(containerManagerBindAddr);    
    }
    ContainerManager proxy =
        user.doAs(new PrivilegedAction<ContainerManager>() {
          @Override
          public ContainerManager run() {
            YarnRPC rpc = YarnRPC.create(getConfig());
            return (ContainerManager) rpc.getProxy(ContainerManager.class,
                NetUtils.createSocketAddr(containerManagerBindAddr),
                getConfig());
          }
        });
    return proxy;
  }

  /**
   * Setup and start the container on remote nodemanager.
   */
  private class EventProcessor implements Runnable {
    private ContainerLauncherEvent event;

    EventProcessor(ContainerLauncherEvent event) {
      this.event = event;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      LOG.info("Processing the event " + event.toString());

      // Load ContainerManager tokens before creating a connection.
      // TODO: Do it only once per NodeManager.
      final String containerManagerBindAddr = event.getContainerMgrAddress();
      ContainerId containerID = event.getContainerID();
      ContainerToken containerToken = event.getContainerToken();

      switch(event.getType()) {

      case CONTAINER_REMOTE_LAUNCH:
        ContainerRemoteLaunchEvent launchEv = (ContainerRemoteLaunchEvent) event;

        TaskAttemptId taskAttemptID = launchEv.getTaskAttemptID();
        try {
          
          ContainerManager proxy = 
            getCMProxy(containerID, containerManagerBindAddr, containerToken);
          
          // Construct the actual Container
          ContainerLaunchContext containerLaunchContext =
              launchEv.getContainer();

          // Now launch the actual container
          StartContainerRequest startRequest = recordFactory
              .newRecordInstance(StartContainerRequest.class);
          startRequest.setContainerLaunchContext(containerLaunchContext);
          StartContainerResponse response = proxy.startContainer(startRequest);
          ByteBuffer portInfo = response
              .getServiceResponse(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID);
          int port = -1;
          if(portInfo != null) {
            port = ShuffleHandler.deserializeMetaData(portInfo);
          }
          LOG.info("Shuffle port returned by ContainerManager for "
              + taskAttemptID + " : " + port);
          
          if(port < 0) {
            throw new IllegalStateException("Invalid shuffle port number "
                + port + " returned for " + taskAttemptID);
          }

          // after launching, send launched event to task attempt to move
          // it from ASSIGNED to RUNNING state
          context.getEventHandler().handle(
              new TaskAttemptContainerLaunchedEvent(taskAttemptID, port));
        } catch (Throwable t) {
          String message = "Container launch failed for " + containerID
              + " : " + StringUtils.stringifyException(t);
          LOG.error(message);
          context.getEventHandler().handle(
              new TaskAttemptDiagnosticsUpdateEvent(taskAttemptID, message));
          context.getEventHandler().handle(
              new TaskAttemptEvent(taskAttemptID,
                  TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED));
        }

        break;

      case CONTAINER_REMOTE_CLEANUP:
        // We will have to remove the launch (meant "cleanup"? FIXME) event if it is still in eventQueue
        // and not yet processed
        if (eventQueue.contains(event)) {
          eventQueue.remove(event); // TODO: Any synchro needed?
          //deallocate the container
          context.getEventHandler().handle(
              new ContainerAllocatorEvent(event.getTaskAttemptID(),
              ContainerAllocator.EventType.CONTAINER_DEALLOCATE));
        } else {
          try {
            ContainerManager proxy = 
              getCMProxy(containerID, containerManagerBindAddr, containerToken);
            // TODO:check whether container is launched

            // kill the remote container if already launched
            StopContainerRequest stopRequest = recordFactory
                .newRecordInstance(StopContainerRequest.class);
            stopRequest.setContainerId(event.getContainerID());
            proxy.stopContainer(stopRequest);

          } catch (Throwable t) {
            //ignore the cleanup failure
            LOG.warn("cleanup failed for container " + event.getContainerID() ,
                t);
          }

          // after killing, send killed event to taskattempt
          context.getEventHandler().handle(
              new TaskAttemptEvent(event.getTaskAttemptID(),
                  TaskAttemptEventType.TA_CONTAINER_CLEANED));
        }
        break;
      }
    }
    
  }

  @Override
  public void handle(ContainerLauncherEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }
}
