package JobScheduler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

enum JobStatus {
    NOT_STARTED,
    SUBMITTED,
    COMPLETED,
    FAILED
}

enum TaskPriority {
    HIGH,
    MEDIUM,
    LOW
}

@FunctionalInterface
interface TaskExecution {

    /*
     * FIX:
     * Input is now actually consumed by the task.
     */
    void run(Object input);
}

class Job {

    int id;
    String name;
    Object input;
    TaskPriority priority;
    TaskExecution execution;
    LocalDateTime startTime;
    Duration totalExecutionTime;

    /*
     * FIX:
     * volatile gives visibility across scheduler
     * and worker threads.
     *
     * synchronized(job) is still used when
     * we need atomic check + update.
     */
    volatile JobStatus status;

    Job(
            int id,
            String name,
            Object input,
            TaskPriority priority,
            TaskExecution execution,
            LocalDateTime startTime) {

        this.id = id;
        this.name = name;
        this.input = input;
        this.priority = priority;
        this.execution = execution;
        this.startTime = startTime;

        this.status =
                JobStatus.NOT_STARTED;
    }

    @Override
    public String toString() {

        return "Job{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", priority=" + priority +
                ", startTime=" + startTime +
                ", status=" + status +
                ", totalExecutionTime=" + totalExecutionTime +
                '}';
    }
}

class JobService {

    ConcurrentHashMap<Integer, Job> jobs =
            new ConcurrentHashMap<>();

    void addJob(Job job) {
        Job existing =
                jobs.putIfAbsent(
                        job.id,
                        job);
    }

    Job getJob(
            int jobId) {

        return jobs.get(jobId);
    }

    ArrayList<Job> getAllJobs() {

        return new ArrayList<>(
                jobs.values());
    }
}


interface JobSchedulingStrategy {

    List<Job> orderJobs(
            List<Job> jobs);
}

/*
 * Earliest scheduled time first.
 */
class EarliestStartTimeStrategy
        implements JobSchedulingStrategy {

    @Override
    public List<Job> orderJobs(
            List<Job> jobs) {

        return jobs
                .stream()
                .sorted(
                        (a, b) ->
                                a.startTime
                                        .compareTo(
                                                b.startTime))
                .toList();
    }
}

/*
 * Higher priority first.
 *
 * If same priority:
 * earlier start time wins.
 */
class PrioritySchedulingStrategy
        implements JobSchedulingStrategy {

    @Override
    public List<Job> orderJobs(
            List<Job> jobs) {

        return jobs
                .stream()
                .sorted((a, b) -> {

                    if (a.priority
                            == b.priority) {

                        return a.startTime
                                .compareTo(
                                        b.startTime);
                    }

                    return Integer.compare(
                            getPriority(
                                    b.priority),
                            getPriority(
                                    a.priority));
                })
                .toList();
    }

    int getPriority(
            TaskPriority priority) {

        if (priority
                == TaskPriority.HIGH) {

            return 3;
        }

        if (priority
                == TaskPriority.MEDIUM) {

            return 2;
        }

        return 1;
    }
}

/
class JobExecutor {

    ExecutorService executor;

    JobExecutor(
            int workerThreads) {

        if (workerThreads <= 0) {

            throw new IllegalArgumentException(
                    "workerThreads must be > 0");
        }

        executor =
                Executors.newFixedThreadPool(
                        workerThreads);
    }

    void execute(
            Job job,
            SchedulerService schedulerService) {

        executor.submit(() -> {

            LocalDateTime actualStart =
                    LocalDateTime.now();

            try {

                System.out.println(
                        Thread.currentThread()
                                .getName()
                                + " executing "
                                + job.name);

                /*
                 * FIX:
                 * Job input is now passed
                 * to execution logic.
                 */
                job.execution.run(
                        job.input);

                Duration duration =
                        Duration.between(
                                actualStart,
                                LocalDateTime.now());

                /*
                 * FIX:
                 * Executor notifies scheduler;
                 * it does not mutate lifecycle state itself.
                 */
                schedulerService
                        .markCompleted(
                                job,
                                duration);

            } catch (Exception exception) {

                Duration duration =
                        Duration.between(
                                actualStart,
                                LocalDateTime.now());

                schedulerService
                        .markFailed(
                                job,
                                duration,
                                exception);
            }
        });
    }

    void shutdown() {

        executor.shutdown();
    }
}

class SchedulerService {

    /*
     * FIX:
     * volatile because strategy may be changed
     * while scheduler thread is running.
     */
    volatile JobSchedulingStrategy strategy;
    JobService jobService;
    JobExecutor executor;

    AtomicBoolean running =
            new AtomicBoolean(false);

    Thread schedulerThread;

    long pollingIntervalMillis = 500;

    SchedulerService(
            JobSchedulingStrategy strategy,
            int workerThreads) {

        if (strategy == null) {

            throw new IllegalArgumentException(
                    "Strategy cannot be null");
        }

        this.strategy =
                strategy;

        this.jobService =
                new JobService();

        this.executor =
                new JobExecutor(
                        workerThreads);
    }

    void addJob(
            Job job) {

        jobService.addJob(
                job);
    }

    void changeStrategy(
            JobSchedulingStrategy strategy) {

        if (strategy == null) {

            throw new IllegalArgumentException(
                    "Strategy cannot be null");
        }

        this.strategy =
                strategy;
    }

    void start() {

        /*
         * Prevent multiple scheduler threads.
         */
        if (!running.compareAndSet(
                false,
                true)) {

            return;
        }

        schedulerThread =
                new Thread(() -> {

                    while (running.get()) {

                        try {

                            ArrayList<Job> allJobs =
                                    jobService
                                            .getAllJobs();

                            /*
                             * Strategy only orders jobs.
                             */
                            List<Job> orderedJobs =
                                    strategy
                                            .orderJobs(
                                                    allJobs);

                            scheduleReadyJobs(
                                    orderedJobs,
                                    LocalDateTime.now());

                            Thread.sleep(
                                    pollingIntervalMillis);

                        } catch (
                                InterruptedException exception) {

                            if (!running.get()) {
                                break;
                            }

                            Thread.currentThread()
                                    .interrupt();

                            break;
                        }
                    }
                });

        schedulerThread.setName(
                "scheduler-thread");

        schedulerThread.start();

        System.out.println(
                "Scheduler started");
    }

    void scheduleReadyJobs(
            List<Job> jobs,
            LocalDateTime currentTime) {

        for (Job job : jobs) {

            /*
             * Not ready yet.
             */
            if (job.startTime
                    .isAfter(currentTime)) {

                continue;
            }

            /*
             * FIX:
             *
             * Per-job lock gives atomic:
             *
             * check state
             * +
             * claim job
             *
             * This prevents duplicate submission.
             */
            synchronized (job) {

                if (job.status
                        != JobStatus.NOT_STARTED) {

                    continue;
                }

                /*
                 * Claim before handing
                 * to executor.
                 */
                job.status =
                        JobStatus.SUBMITTED;
            }

            executor.execute(
                    job,
                    this);
        }
    }

    /*
     * FIX:
     * Job lifecycle belongs to SchedulerService.
     */
    void markCompleted(
            Job job,
            Duration duration) {

        synchronized (job) {

            job.totalExecutionTime =
                    duration;

            job.status =
                    JobStatus.COMPLETED;
        }

        System.out.println(
                "Completed: "
                        + job.name);
    }

    /*
     * FIX:
     * Failure transition also owned
     * by SchedulerService.
     */
    void markFailed(
            Job job,
            Duration duration,
            Exception exception) {

        synchronized (job) {

            job.totalExecutionTime =
                    duration;

            job.status =
                    JobStatus.FAILED;
        }

        System.out.println(
                "Failed: "
                        + job.name
                        + ", reason="
                        + exception.getMessage());
    }

    JobStatus getStatus(
            int jobId) {

        Job job =
                jobService.getJob(
                        jobId);

        if (job == null) {

            throw new RuntimeException(
                    "Job not found: "
                            + jobId);
        }

        return job.status;
    }

    Job getJob(
            int jobId) {

        Job job =
                jobService.getJob(
                        jobId);

        if (job == null) {

            throw new RuntimeException(
                    "Job not found: "
                            + jobId);
        }

        return job;
    }

    void stop() {

        running.set(false);

        if (schedulerThread != null) {

            schedulerThread.interrupt();
        }

        executor.shutdown();

        System.out.println(
                "Scheduler stopped");
    }
}

class JobSchedulerFacade {

    SchedulerService schedulerService;

    JobSchedulerFacade(
            SchedulerService schedulerService) {

        this.schedulerService =
                schedulerService;
    }

    void scheduleJob(
            Job job) {

        schedulerService
                .addJob(job);
    }

    void changeStrategy(
            JobSchedulingStrategy strategy) {

        schedulerService
                .changeStrategy(
                        strategy);
    }

    JobStatus getJobStatus(
            int jobId) {

        return schedulerService
                .getStatus(jobId);
    }

    Job getJob(
            int jobId) {

        return schedulerService
                .getJob(jobId);
    }

    void start() {

        schedulerService.start();
    }

    void stop() {

        schedulerService.stop();
    }
}

public class Main {

    public static void main(
            String[] args)
            throws Exception {

        /*
         * Initial strategy:
         * earliest start time first.
         */
        JobSchedulingStrategy strategy =
                new EarliestStartTimeStrategy();

        /*
         * Maximum 3 jobs execute concurrently.
         */
        SchedulerService schedulerService =
                new SchedulerService(
                        strategy,
                        3);

        JobSchedulerFacade scheduler =
                new JobSchedulerFacade(
                        schedulerService);

        scheduler.start();


        /*
         * JOB 1
         */
        Job emailJob =
                new Job(
                        1,
                        "Send Email",
                        "user@example.com",
                        TaskPriority.LOW,

                        input -> {

                            String email =
                                    (String) input;

                            System.out.println(
                                    "Sending email to "
                                            + email);

                            sleep(2000);
                        },

                        LocalDateTime
                                .now()
                                .plusSeconds(2));


        /*
         * JOB 2
         */
        Job paymentJob =
                new Job(
                        2,
                        "Process Payment",
                        5000,
                        TaskPriority.HIGH,

                        input -> {

                            int amount =
                                    (Integer) input;

                            System.out.println(
                                    "Processing payment: "
                                            + amount);

                            sleep(3000);
                        },

                        LocalDateTime
                                .now()
                                .plusSeconds(2));


        /*
         * JOB 3
         */
        Job reportJob =
                new Job(
                        3,
                        "Generate Report",
                        "MONTHLY",
                        TaskPriority.MEDIUM,

                        input -> {

                            System.out.println(
                                    "Generating "
                                            + input
                                            + " report");

                            sleep(1000);
                        },

                        LocalDateTime
                                .now()
                                .plusSeconds(2));


        scheduler.scheduleJob(
                emailJob);

        scheduler.scheduleJob(
                paymentJob);

        scheduler.scheduleJob(
                reportJob);


        Thread.sleep(6000);


        System.out.println(
                "\nStatuses:");

        System.out.println(
                scheduler.getJob(1));

        System.out.println(
                scheduler.getJob(2));

        System.out.println(
                scheduler.getJob(3));


        /*
         * Change scheduling strategy.
         */
        System.out.println(
                "\nChanging to Priority Strategy\n");

        scheduler.changeStrategy(
                new PrioritySchedulingStrategy());


        LocalDateTime nextStart =
                LocalDateTime
                        .now()
                        .plusSeconds(1);


        Job lowPriorityJob =
                new Job(
                        4,
                        "Low Priority Task",
                        null,
                        TaskPriority.LOW,

                        input -> {

                            System.out.println(
                                    "Executing low priority task");

                            sleep(1000);
                        },

                        nextStart);


        Job highPriorityJob =
                new Job(
                        5,
                        "High Priority Task",
                        null,
                        TaskPriority.HIGH,

                        input -> {

                            System.out.println(
                                    "Executing high priority task");

                            sleep(1000);
                        },

                        nextStart);


        Job mediumPriorityJob =
                new Job(
                        6,
                        "Medium Priority Task",
                        null,
                        TaskPriority.MEDIUM,

                        input -> {

                            System.out.println(
                                    "Executing medium priority task");

                            sleep(1000);
                        },

                        nextStart);


        scheduler.scheduleJob(
                lowPriorityJob);

        scheduler.scheduleJob(
                highPriorityJob);

        scheduler.scheduleJob(
                mediumPriorityJob);


        Thread.sleep(4000);

        scheduler.stop();
    }

    static void sleep(
            long millis) {

        try {

            Thread.sleep(
                    millis);

        } catch (
                InterruptedException exception) {

            Thread.currentThread()
                    .interrupt();
        }
    }
}