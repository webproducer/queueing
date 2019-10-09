<?php
namespace Queueing\Tests;

use Amp\Deferred;
use Amp\Delayed;
use Amp\Loop;
use Amp\Success;
use PHPUnit\Framework\TestCase;
use Queueing\AsyncQueueProcessor;
use Queueing\JobInterface;
use Queueing\JobPerformerInterface;
use Queueing\JobsQueueInterface;
use Queueing\PerformingException;
use function Amp\call;

class AsyncQueueProcessorTest extends TestCase
{
    /**
     * @dataProvider dataProviderProcess
     * @param array $jobsIds
     * @param array $deletedJobsIds
     * @param array $buriedJobsIds
     * @param callable $performerCallback
     * @param int $bulkSize
     * @param int $loopTimeout
     */
    public function testProcess(
        array $jobsIds,
        array $deletedJobsIds,
        array $buriedJobsIds,
        callable $performerCallback,
        int $bulkSize,
        int $loopTimeout
    ) {
        $jobIdWrapIntoArray = function (int $jobId) {
            return [$jobId];
        };
        $jobsQueue = $this->createMock(JobsQueueInterface::class);
        $jobsQueue->expects($this->atLeast(count($jobsIds)))
            ->method('reserve')
            ->willReturnCallback(function (int $timeout) use (&$jobsIds) {
                return call(function () use (&$jobsIds, $timeout) {
                    yield new Delayed(rand(1, 5));
                    $jobId = array_shift($jobsIds);
                    return is_null($jobId) ? yield new Delayed($timeout, null) : [$jobId, ''];
                });
            });
        $jobsQueue->expects($this->exactly(count($deletedJobsIds)))
            ->method('delete')
            ->withConsecutive(...array_map($jobIdWrapIntoArray, $deletedJobsIds))
            ->willReturn(new Success());
        $jobsQueue->expects($this->exactly(count($buriedJobsIds)))
            ->method('bury')
            ->withConsecutive(...array_map($jobIdWrapIntoArray, $buriedJobsIds))
            ->willReturn(new Success());

        $performer = $this->createMock(JobPerformerInterface::class);
        $performer->expects($this->exactly(count($jobsIds)))
            ->method('perform')
            ->willReturnCallback($performerCallback);
        $processor = new AsyncQueueProcessor($performer);
        Loop::run(function () use ($processor, $jobsQueue, $bulkSize, $loopTimeout) {
            (new Delayed($loopTimeout))->onResolve(function () use ($processor) {
                $processor->stop();
            });
            yield $processor->process($jobsQueue, $bulkSize, 10);
        });
    }

    public function dataProviderProcess(): array
    {
        return [
            '1 job deleted (bulk:1)' => [
                'jobsIds' => [
                    1,
                ],
                'deletedJobsIds' => [
                    1,
                ],
                'buriedJobsIds' => [],
                'performerCallback' => function () {
                    return call(function () {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def) {
                            $def->resolve();
                        });
                        return $def->promise();
                    });
                },
                'bulkSize' => 1,
                'loopTimeout' => 200,
            ],
            '1 job buried (bulk:1)' => [
                'jobsIds' => [
                    1,
                ],
                'deletedJobsIds' => [
                ],
                'buriedJobsIds' => [1],
                'performerCallback' => function () {
                    return call(function () {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def) {
                            $def->fail(new PerformingException());
                        });
                        return $def->promise();
                    });
                },
                'bulkSize' => 1,
                'loopTimeout' => 200,
            ],
            '2 jobs deleted (bulk:1)' => [
                'jobsIds' => [
                    1, 2,
                ],
                'deletedJobsIds' => [
                    1, 2,
                ],
                'buriedJobsIds' => [],
                'performerCallback' => function () {
                    return call(function () {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def) {
                            $def->resolve();
                        });
                        return $def->promise();
                    });
                },
                'bulkSize' => 1,
                'loopTimeout' => 200,
            ],
            '2 jobs buried (bulk:1)' => [
                'jobsIds' => [1, 2],
                'deletedJobsIds' => [
                ],
                'buriedJobsIds' => [1, 2],
                'performerCallback' => function () {
                    return call(function () {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def) {
                            $def->fail(new PerformingException());
                        });
                        return $def->promise();
                    });
                },
                'bulkSize' => 1,
                'loopTimeout' => 200,
            ],
            '2 jobs mixed (bulk:1)' => [
                'jobsIds' => [1, 2],
                'deletedJobsIds' => [1],
                'buriedJobsIds' => [2],
                'performerCallback' => function (JobInterface $job) {
                    return call(function () use ($job) {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def, $job) {
                            if ($job->getId() === 1) {
                                $def->resolve();
                            } else {
                                $def->fail(new PerformingException());
                            }
                        });
                        return $def->promise();
                    });
                },
                'bulkSize' => 1,
                'loopTimeout' => 200,
            ],
            '20 jobs deleted (bulk:1)' => [
                'jobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
                'deletedJobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
                'buriedJobsIds' => [],
                'performerCallback' => function () {
                    return call(function () {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def) {
                            $def->resolve();
                        });
                        return $def->promise();
                    });
                },
                'bulkSize' => 1,
                'loopTimeout' => 7500,
            ],
            '20 jobs buried (bulk:1)' => [
                'jobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
                'deletedJobsIds' => [],
                'buriedJobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
                'performerCallback' => function (JobInterface $job) {
                    return call(function (JobInterface $job) {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def, $job) {
                            $def->fail((new PerformingException("Job#{$job->getId()}"))->setJob($job));
                        });
                        return $def->promise();
                    }, $job);
                },
                'bulkSize' => 1,
                'loopTimeout' => 7500,
            ],
            '20 jobs mixed (bulk:1)' => [
                'jobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
                'deletedJobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19],
                'buriedJobsIds' => [10, 20],
                'performerCallback' => function (JobInterface $job) {
                    return call(function (JobInterface $job) {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def, $job) {
                            if (($job->getId() % 10) === 0) {
                                $def->fail((new PerformingException("Job#{$job->getId()}"))->setJob($job));
                            } else {
                                $def->resolve();
                            }
                        });
                        return $def->promise();
                    }, $job);
                },
                'bulkSize' => 1,
                'loopTimeout' => 7500,
            ],
            '5 jobs deleted (bulk:5)' => [
                'jobsIds' => [1, 2, 3, 4, 5],
                'deletedJobsIds' => [1, 2, 3, 4, 5],
                'buriedJobsIds' => [],
                'performerCallback' => function () {
                    return call(function () {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def) {
                            $def->resolve();
                        });
                        return $def->promise();
                    });
                },
                'bulkSize' => 5,
                'loopTimeout' => 200,
            ],
            '5 jobs buried (bulk:5)' => [
                'jobsIds' => [1, 2, 3, 4, 5],
                'deletedJobsIds' => [],
                'buriedJobsIds' => [1, 2, 3, 4, 5],
                'performerCallback' => function (JobInterface $job) {
                    return call(function (JobInterface $job) {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def, $job) {
                            $def->fail((new PerformingException("Job#{$job->getId()}"))->setJob($job));
                        });
                        return $def->promise();
                    }, $job);
                },
                'bulkSize' => 5,
                'loopTimeout' => 200,
            ],
            '5 jobs mixed (bulk:5)' => [
                'jobsIds' => [1, 2, 3, 4, 5],
                'deletedJobsIds' => [1, 3, 5],
                'buriedJobsIds' => [2, 4],
                'performerCallback' => function (JobInterface $job) {
                    return call(function (JobInterface $job) {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def, $job) {
                            if (($job->getId() % 2) === 0) {
                                $def->fail((new PerformingException("Job#{$job->getId()}"))->setJob($job));
                            } else {
                                $def->resolve();
                            }
                        });
                        return $def->promise();
                    }, $job);
                },
                'bulkSize' => 5,
                'loopTimeout' => 200,
            ],
            '10 jobs deleted (bulk:5)' => [
                'jobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                'deletedJobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                'buriedJobsIds' => [],
                'performerCallback' => function () {
                    return call(function () {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def) {
                            $def->resolve();
                        });
                        return $def->promise();
                    });
                },
                'bulkSize' => 5,
                'loopTimeout' => 200,
            ],
            '10 jobs buried (bulk:5)' => [
                'jobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                'deletedJobsIds' => [],
                'buriedJobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                'performerCallback' => function (JobInterface $job) {
                    return call(function (JobInterface $job) {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def, $job) {
                            $def->fail((new PerformingException("Job#{$job->getId()}"))->setJob($job));
                        });
                        return $def->promise();
                    }, $job);
                },
                'bulkSize' => 5,
                'loopTimeout' => 200,
            ],
            '10 jobs mixed (bulk:5)' => [
                'jobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                'deletedJobsIds' => [1, 3, 5, 7, 9],
                'buriedJobsIds' => [2, 4, 6, 8, 10],
                'performerCallback' => function (JobInterface $job) {
                    return call(function (JobInterface $job) {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def, $job) {
                            if (($job->getId() % 2) === 0) {
                                $def->fail((new PerformingException("Job#{$job->getId()}"))->setJob($job));
                            } else {
                                $def->resolve();
                            }
                        });
                        return $def->promise();
                    }, $job);
                },
                'bulkSize' => 5,
                'loopTimeout' => 200,
            ],
            '12 jobs deleted (bulk:5)' => [
                'jobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                'deletedJobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                'buriedJobsIds' => [],
                'performerCallback' => function () {
                    return call(function () {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def) {
                            $def->resolve();
                        });
                        return $def->promise();
                    });
                },
                'bulkSize' => 5,
                'loopTimeout' => 3000,
            ],
            '12 jobs buried (bulk:5)' => [
                'jobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                'deletedJobsIds' => [],
                'buriedJobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                'performerCallback' => function (JobInterface $job) {
                    return call(function (JobInterface $job) {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def, $job) {
                            $def->fail((new PerformingException("Job#{$job->getId()}"))->setJob($job));
                        });
                        return $def->promise();
                    }, $job);
                },
                'bulkSize' => 5,
                'loopTimeout' => 3000,
            ],
            '12 jobs mixed (bulk:5)' => [
                'jobsIds' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                'deletedJobsIds' => [1, 3, 5, 7, 9, 11],
                'buriedJobsIds' => [2, 4, 6, 8, 10, 12],
                'performerCallback' => function (JobInterface $job) {
                    return call(function (JobInterface $job) {
                        $def = new Deferred();
                        (new Delayed(400))->onResolve(function () use ($def, $job) {
                            if (($job->getId() % 2) === 0) {
                                $def->fail((new PerformingException("Job#{$job->getId()}"))->setJob($job));
                            } else {
                                $def->resolve();
                            }
                        });
                        return $def->promise();
                    }, $job);
                },
                'bulkSize' => 5,
                'loopTimeout' => 3000,
            ],
        ];
    }
}
