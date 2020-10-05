package org.example.jet.petclinic;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.picocli.CommandLine;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

public class PetClinicIndexingApp {

    private static final ILogger log = Logger.getLogger(PetClinicIndexingApp.class);

    public static void main(String[] args) throws Exception {
        PetClinicIndexJob petClinicIndexJob = new PetClinicIndexJob();
        new CommandLine(petClinicIndexJob).parseArgs(args);

        JetInstance jet = Jet.bootstrappedInstance();

        log.info("Submitting PetClinicIndexJob");

        Job job = jet.newJob(petClinicIndexJob.pipeline(), petClinicIndexJob.config());

        while (job.getStatus() == JobStatus.NOT_RUNNING || job.getStatus() == JobStatus.STARTING) {
            log.info("PetClinicIndexJob status=" + job.getStatus());
            Thread.sleep(1000);
        }

        log.info("PetClinicIndexJob status=" + job.getStatus());
    }


}
