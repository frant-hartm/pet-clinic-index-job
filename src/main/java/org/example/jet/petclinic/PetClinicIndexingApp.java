package org.example.jet.petclinic;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.picocli.CommandLine;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.example.jet.petclinic.rake.Rake;

public class PetClinicIndexingApp {

    private static final ILogger log = Logger.getLogger(PetClinicIndexingApp.class);

    public static void main(String[] args) throws Exception {
        PetClinicIndexJob petClinicIndexJob = new PetClinicIndexJob();
        new CommandLine(petClinicIndexJob).parseArgs(args);
        Pipeline pipeline = petClinicIndexJob.pipeline();

        JetInstance jet = Jet.bootstrappedInstance();

        log.info("Submitting PetClinicIndexJob");

        JobConfig jobConfig = new JobConfig();
        jobConfig.addPackage("data");
        jobConfig.addClass(Rake.class);
        jobConfig.addPackage("picocli");
        jobConfig.addClass(PetClinicIndexJob.class);

        Job job = jet.newJob(pipeline, jobConfig.setName("PetClinicIndexJob"));

        while (job.getStatus() == JobStatus.NOT_RUNNING || job.getStatus() == JobStatus.STARTING) {
            log.info("PetClinicIndexJob status=" + job.getStatus());
            Thread.sleep(1000);
        }

        log.info("PetClinicIndexJob status=" + job.getStatus());
    }


}
