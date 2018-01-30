<?php
/**
 * Created by PhpStorm.
 * User: sashok
 * Date: 05.12.2017
 * Time: 16:25
 */

namespace Queueing;


interface JobFactoryInterface
{

    /**
     * @param int $id
     * @param string $payload
     * @return JobInterface
     */
    function makeJob(int $id, string $payload): JobInterface;

}
