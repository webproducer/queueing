<?php
namespace Queueing;


interface IJobPerformer
{

    /**
     * @param IJob $job
     */
    function perform(IJob $job);

}
