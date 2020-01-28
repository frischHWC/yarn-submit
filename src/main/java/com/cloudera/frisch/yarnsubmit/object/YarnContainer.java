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

