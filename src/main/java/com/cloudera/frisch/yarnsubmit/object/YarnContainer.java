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
package com.cloudera.frisch.yarnsubmit.object;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@ToString
public class YarnContainer {
    String command;
    boolean successful;
    int tries;
    state finalState;
    Long containerId;

    public YarnContainer(String command) {
        this.command = command;
        this.successful = false;
        this.tries = 0;
        this.finalState = state.TO_RUN;
    }

    public YarnContainer(String command, Long containerId) {
        this.command = command;
        this.successful = false;
        this.tries = 1;
        this.finalState = state.TO_RUN;
        this.containerId = containerId;
    }

    public enum state {
        TO_RUN,
        RUNNING,
        FINISHED
    }

    public void setContainerRunning(Long containerId) {
        this.containerId = containerId;
        this.finalState = state.RUNNING;
        this.tries++;
    }

    public void resetContainerToRun() {
        this.containerId = 0L;
        this.finalState = state.TO_RUN;
    }

    public static YarnContainer findContainerUsingContainerId(List<YarnContainer> containers, Long id) {
        for(YarnContainer yarnContainer: containers) {
            if(yarnContainer.containerId.equals(id)) {
                return yarnContainer;
            }
        }
        return null;
    }

    public static YarnContainer findOneAvailableCommandToLaunch(List<YarnContainer> containers) {
        for(YarnContainer container: containers) {
            if(container.finalState == state.TO_RUN) {
                return container;
            }
        }
        return null;
    }

    public static Integer getNumberOfCompletedContainers(List<YarnContainer> containers) {
        int containersCompleted = 0;
        for(YarnContainer container: containers) {
            if(container.finalState==state.FINISHED){
                containersCompleted++;
            }
        }
        return containersCompleted;
    }

    public static boolean checkAllContainersAreSuccessful(List<YarnContainer> containers) {
        for(YarnContainer container: containers) {
            if(!container.isSuccessful()) {
                return false;
            }
        }
        return true;
    }

}

