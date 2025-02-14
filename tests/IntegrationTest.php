<?php

namespace Queueing\Tests;

use Amp\Delayed;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Process\Process;
use Amp\Process\ProcessException;
use Amp\Promise;
use Pheanstalk\Exception\DeadlineSoonException;
use Psr\Log\Test\TestLogger;
use Queueing\AsyncQueueProcessor;
use Queueing\Backends\Beanstalk\AsyncQueue;
use Queueing\Backends\Beanstalk\Queue;
use Queueing\BaseFactory;
use Queueing\JobInterface;
use Queueing\JobPerformerInterface;
use Queueing\JobsQueueException;
use function Amp\call;

class IntegrationTest extends AsyncTestCase
{
    /** @var Process */
    private $beanstalkd;
    /** @var Queue */
    private $queue;

    /**
     * @throws JobsQueueException
     * @throws DeadlineSoonException
     */
    public function setUpAsync() {
        $this->beanstalkd = new Process('beanstalkd -l 127.0.0.1');
        yield $this->beanstalkd->start();

        if (!$this->queue) {
            $this->queue = new Queue();
        }
        $this->drainQueue();
    }

    /**
     * @throws ProcessException
     */
    public function tearDownAsync() {
        if ($this->beanstalkd) {
            echo yield $this->beanstalkd->getStdout()->read();
            if ($this->beanstalkd->isRunning()) {
                $this->beanstalkd->signal(SIGTERM);
                $exitCode = yield $this->beanstalkd->join();
                printf("Beanstalkd exit code: %s\n", $exitCode);
            }
        }
    }

    /**
     * @dataProvider dataProviderProcessQueue
     *
     * @param Job[] $jobs
     * @param int $stopDelay
     * @param int $loopTimeout
     * @param int $bulkSize
     * @param array $processedJobsIDs
     * @param array $remainedJobsIDs
     * @return \Generator
     * @throws DeadlineSoonException
     * @throws JobsQueueException
     */
    public function testProcessQueue(array $jobs, int $stopDelay, int $loopTimeout, int $bulkSize, array $processedJobsIDs, array $remainedJobsIDs) {
        // Arrange
        foreach ($jobs as $job) {
            $this->queue->add($job->getPayload());
        }

        $performer = new class implements JobPerformerInterface {
            private $jobsIDs = [];

            public function perform(JobInterface $job): Promise {
                return call(function (JobInterface $job) {
                    if (!$job instanceof Job) {
                        throw new \RuntimeException("Job must be an instance of %s", Job::class);
                    }
                    $this->jobsIDs[] = $job->getPayloadId();
                    yield new Delayed($job->getDuration());
                }, $job);
            }

            public function getJobsIDs(): array {
                return $this->jobsIDs;
            }
        };
        $logger = new TestLogger();

        $processor = new AsyncQueueProcessor($performer, new BaseFactory(Job::class), $logger);
        $stop = call(function () use ($processor, $stopDelay) {
            yield new Delayed($stopDelay);
            $processor->stop();
        });
        $this->setTimeout($loopTimeout);

        // Act
        yield Promise\all([
            $processor->process(new AsyncQueue(), $bulkSize),
            $stop
        ]);

        // Assert
        $jobsIDs = $performer->getJobsIDs();
        $this->assertEquals($processedJobsIDs, $jobsIDs);
        $this->assertEquals($remainedJobsIDs, $this->drainQueue());
        if ($logger->hasWarningRecords()) {
            // Fail if there is any internal exception during the loop.
            $this->fail(sprintf(
                "Unexpected internal exception(s):\n%s",
                implode("\n", $this->getMessagesForLevel($logger, 'warning'))
            ));
        }
    }

    public function dataProviderProcessQueue(): array {
        return [
            'bulk 1, job 1' => [
                'jobs' => [Job::create(1, 50)],
                'stopDelay' => 40, // Stop processor after 40 milliseconds
                'loopTimeout' => 400, // Hangup the loop after 200 milliseconds
                'bulkSize' => 1,
                'processedJobsIDs' => [1],
                'remainedJobsIDs' => [],
            ],
            'bulk 1, jobs 2' => [
                'jobs' => [Job::create(1, 50), Job::create(2, 50)],
                'stopDelay' => 40, // Stop processor after 40 milliseconds
                'loopTimeout' => 200, // Hangup the loop after 200 milliseconds
                'bulkSize' => 1,
                'processedJobsIDs' => [1],
                'remainedJobsIDs' => [2],
            ],
            'bulk 1, jobs 3' => [
                'jobs' => [Job::create(1, 50), Job::create(2, 50), Job::create(3, 50)],
                'stopDelay' => 110,
                'loopTimeout' => 200,
                'bulkSize' => 1,
                'processedJobsIDs' => [1, 2],
                'remainedJobsIDs' => [3],
            ],
            'bulk 2, job 1' => [
                'jobs' => [Job::create(1, 50)],
                'stopDelay' => 40,
                'loopTimeout' => 200,
                'bulkSize' => 2,
                'processedJobsIDs' => [1],
                'remainedJobsIDs' => [],
            ],
            'bulk 2, jobs 2' => [
                'jobs' => [Job::create(1, 50), Job::create(2, 50)],
                'stopDelay' => 40,
                'loopTimeout' => 200,
                'bulkSize' => 2,
                'processedJobsIDs' => [1, 2],
                'remainedJobsIDs' => [],
            ],
            'bulk 2, jobs 3' => [
                'jobs' => [Job::create(1, 50), Job::create(2, 50), Job::create(3, 50)],
                'stopDelay' => 40,
                'loopTimeout' => 200,
                'bulkSize' => 2,
                'processedJobsIDs' => [1, 2],
                'remainedJobsIDs' => [3],
            ],
            'bulk 2, jobs 5' => [
                'jobs' => [
                    Job::create(1, 50),
                    Job::create(2, 50),
                    Job::create(3, 50),
                    Job::create(4, 50),
                    Job::create(5, 50),
                ],
                'stopDelay' => 90,
                'loopTimeout' => 250,
                'bulkSize' => 2,
                'processedJobsIDs' => [1, 2, 3, 4],
                'remainedJobsIDs' => [5],
            ],
        ];
    }


    /**
     * @throws JobsQueueException
     * @throws DeadlineSoonException
     */
    private function drainQueue(): array {
        $jobsIDs = [];
        do {
            [$jobId, $jobData] = $this->queue->reserve(0);
            if ($jobId !== 0) {
                $this->queue->delete($jobId);
                $job = Job::createWithIdAndPayload($jobId, $jobData);
                try {
                    $jobsIDs[] = $job->getPayloadId();
                } catch (\Throwable $exception) {
                }
            }
        } while ($jobData !== null);

        return $jobsIDs;
    }

    public function getMessagesForLevel(TestLogger $logger, string $level): array {
        return array_map(function ($record) use ($logger, $level) {
            return $record['message'];
        }, $logger->recordsByLevel[$level]);
    }
}