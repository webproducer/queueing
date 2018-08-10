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
            while ($jobData = yield $this->nextJob($this->waitTime)) {
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

    private function makeList(array $jobDescs): Bulk
    {
        return new Bulk(array_map(function($jobDesc) {
            //TODO: handle JobCreatingException?
            return $this->makeJob($jobDesc);
        }, $jobDescs));
    }

}
