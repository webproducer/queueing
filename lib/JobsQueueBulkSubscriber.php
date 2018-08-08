<?php
namespace Queueing;

use Amp\{ Promise, Delayed };

use function Amp\{ call, asyncCall };
use function Amp\Promise\first;


/**
 * Class JobsQueueBulkSubscriber
 * @package Queueing
 */
class JobsQueueBulkSubscriber extends AbstractJobsQueueSubscriber
{

    const TIMED_OUT = 'TIMED_OUT';

    private $portion = 1;
    private $waitTime = 0;
    private $jobWaitPromise;

    public function setBulkSize(int $size): self
    {
        $this->portion = $size;
        return $this;
    }

    public function setBulkMaxWaitTime(int $milliseconds): self
    {
        $this->waitTime = $milliseconds;
        return $this;
    }

    public function subscribe(): Subscription
    {
        asyncCall(function () {
            $jobs = [];
            while ($jobData = yield $this->waitJobWithTimeout()) {
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

    private function waitJobWithTimeout(): Promise
    {
        return call(function() {
            if (!$this->jobWaitPromise) {
                $this->jobWaitPromise = $this->nextJob();
            }
            $flag = yield first([
                $this->jobWaitPromise,
                new Delayed($this->waitTime, self::TIMED_OUT)
            ]);
            if ($flag !== TIMEOUT_FLAG) {
                $this->jobWaitPromise = null;
            }
            return $flag;
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
