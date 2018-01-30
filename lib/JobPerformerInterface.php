<?php
namespace Queueing;


interface JobPerformerInterface
{

    /**
     * @param JobInterface $job
     */
    function perform(JobInterface $job);

}
