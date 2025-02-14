<?php

namespace Queueing;

use function Amp\asyncCall;

class JobsQueueSubscriber extends AbstractJobsQueueSubscriber
{
    /**
     * @return Subscription
     */
    public function subscribe(): Subscription {
        asyncCall(function () {
            while (!$this->isStopped()) {
                $jobData = yield $this->nextJob($this->waitTime);
                if (is_array($jobData)) {
                    yield $this->emit($this->makeJob($jobData));
                }
                yield $this->processResults();
            }
            yield $this->complete();
        });

        return $this->makeSubscription();
    }
}
