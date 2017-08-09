<?php
namespace Queueing;

interface IJob
{
    /**
     * @param int $id
     * @param string|null $payload
     * @return static
     */
    static function createWithIdAndPayload($id, $payload = null);

    /**
     * @return array|null
     */
    function getPayload();

    /**
     * @return int
     */
    function getId();

    function perform();

}
