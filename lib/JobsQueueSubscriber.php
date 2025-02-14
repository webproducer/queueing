<?php

namespace Queueing;

use Amp\Deferred;
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
                    $resultWasSent = new Deferred();
                    /**
                     * Send job and deferred to AsyncQueueProcessor
                     * @see AsyncQueueProcessor::process()
                     */
                    yield $this->emit([$this->makeJob($jobData), $resultWasSent]);
                    // Wait for the result was sent
                    yield $resultWasSent->promise();
                    yield $this->processResults();
                }
            }
            yield $this->complete();
        });

        return $this->makeSubscription();
    }
}
