<?php
namespace Queueing;


interface IJobsQueue
{

    /**
     * @param null|int $timeout In seconds
     * @return IJob|null
     */
    function reserve($timeout = null);

    /**
     * @param IJob $job
     */
    function add(IJob $job);

    /**
     * @param IJob $job
     * @return mixed
     */
    function delete(IJob $job);

    /**
     * @param IJob $job
     * @return mixed
     */
    function bury(IJob $job);

}
