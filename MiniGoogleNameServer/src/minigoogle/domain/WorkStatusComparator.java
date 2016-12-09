package minigoogle.domain;

import java.util.Comparator;

public class WorkStatusComparator implements Comparator<WorkerStatus> {

    @Override
    public int compare(WorkerStatus o1, WorkerStatus o2) {

        return o1.getServerUseLiveBeat().compareTo(o2.getLastAliveBeat());

    }
}
