<?php
namespace Queueing;

use Amp\{Beanstalk\TimedOutException, Promise, Delayed};

use function Amp\{ call, asyncCall };


/**
 * Class JobsQueueBulkSubscriber
 * @package Queueing
 */
class JobsQueueBulkSubscriber extends AbstractJobsQueueSubscriber
{

    const TIMED_OUT = 'TIMED_OUT';

    private $portion = 1;
    private $waitTime = 0;

    public function setBulkSize(int $size): self
    {
        $this->portion = $size;
        return $this;
    }

    public function setBulkMaxWaitTime(int $milliseconds): self
    {
        $this->waitTime = intval(round($milliseconds/1000));
        if ($this->waitTime < 1) {
            $this->waitTime = 1;
        }
        return $this;
    }

    public function subscribe(): Subscription
    {
        asyncCall(function () {
            $jobs = [];
            while ($jobData = yield $this->nextJobWithDeadline()) {
                if ($jobData !== self::TIMED_OUT) {
                    $jobs[] = $jobData;
                }
                $jobsCnt = count($jobs);
                if ($jobsCnt && (($jobsCnt === $this->portion) || ($jobData === self::TIMED_OUT))) {
                    yield $this->emitAndProcess($this->makeList($jobs));
                    $jobs = [];
                }
            }
            if (!empty($jobs)) {
                yield $this->emitAndProcess($this->makeList($jobs));
            }
            $this->complete();
        });
        return $this->makeSubscription();
    }

    private function nextJobWithDeadline(): Promise
    {
        return call(function() {
            $result = null;
            $this->nextJob($this->waitTime)->onResolve(function($e, $value) use (&$result) {
                $result = $e ?: $value;
            });
            while (is_null($result)) {
                yield new Delayed(50);
            }
            switch (true) {
                case $result instanceof TimedOutException:
                    return self::TIMED_OUT;
                case $result instanceof \Throwable:
                    throw $result;
                default:
                    return $result;
            }
        });
    }

    private function makeList(array $jobDescs): Bulk
    {
        return new Bulk(array_map(function($jobDesc) {
            //TODO: handle JobCreatingException?
            return $this->makeJob($jobDesc);
        }, $jobDescs));
    }

}
