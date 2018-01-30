<?php
namespace Queueing;


interface JobPerformerInterface
{

    /**
     * @param JobInterface $job
     * @throws PerformingException
     */
    public function perform(JobInterface $job);

}
