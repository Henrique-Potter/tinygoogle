package minigoogle.domain;

import java.time.Instant;
import java.time.LocalDateTime;

public class WorkerStatus {

    private String workerType = ""; // M->Mapper, R->Reducer, MR-> Mapper and Reducer
    private String workerCurrentStatus = "";   //I -> Idle, B->Busy
    private String ipAddress = "";
    private String portNumber = "";
    private String serverOwner = "";
    private Instant serverUseLiveBeat;
    private Instant lastAliveBeat;

    public int NumberofTimesUtilized = 0;

    public Instant getServerUseLiveBeat() {
        return serverUseLiveBeat;
    }

    public void setServerUseLiveBeat() {
        this.serverUseLiveBeat =  Instant.now();
    }

    public Instant getLastAliveBeat() {
        return lastAliveBeat;
    }

    public void setLastAliveBeat() {
        this.lastAliveBeat = Instant.now();
    }

    public String getWorkerType() {
        return workerType;
    }

    public void setWorkerType(String workerType) {
        this.workerType = workerType;
    }

    public String getWorkerCurrentStatus() {
        return workerCurrentStatus;
    }

    public void setWorkerCurrentStatus(String workerCurrentStatus) {
        this.workerCurrentStatus = workerCurrentStatus;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getPortNumber() {
        return portNumber;
    }

    public void setPortNumber(String portNumber) {
        this.portNumber = portNumber;
    }

    public String getServerOwner() {
        return serverOwner;
    }

    public void setServerOwner(String serverOwner) {
        this.serverOwner = serverOwner;
    }
}
