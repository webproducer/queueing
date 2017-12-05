<?php
namespace Queueing;

interface IJob
{
    /**
     * @param int $id
     * @param string $payload
     * @return static
     */
    static function createWithIdAndPayload(int $id, string $payload);

    /**
     * @return string
     */
    function getPayload(): string;

    /**
     * @return int
     */
    function getId(): int;


}
