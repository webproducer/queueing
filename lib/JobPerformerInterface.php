<?php
namespace Queueing;

use Amp\Promise;

interface JobPerformerInterface
{

    /**
     * @param JobInterface $job
     * @return void|Promise
     * @throws PerformingException
     */
    public function perform(JobInterface $job);

}
