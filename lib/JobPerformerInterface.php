<?php
namespace Queueing;

use Amp\Promise;


interface JobPerformerInterface
{

    /**
     * @param JobInterface $job
     * @returns void|Promise
     * @throws PerformingException
     */
    public function perform(JobInterface $job);

}
