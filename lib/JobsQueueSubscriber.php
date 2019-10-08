<?php

namespace Queueing;

use function Amp\asyncCall;

class JobsQueueSubscriber extends AbstractJobsQueueSubscriber
{
    /**
     * @return Subscription
     */
    public function subscribe(): Subscription
    {
        asyncCall(function () {
            while ($jobData = yield $this->nextJob($this->waitTime)) {
                if ($jobData !== self::TIMED_OUT) {
                    yield $this->emit($this->makeJob($jobData));
                }
                yield $this->processResults();
            }
            yield $this->complete();
        });
        return $this->makeSubscription();
    }
}
