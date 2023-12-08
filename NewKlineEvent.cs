private async Task NewKlineEvent(object obj, string e)
{
    var redisKey = "crypto:binance:last_kline:{0}";
    var k = JsonSerializer.Deserialize<KlineStatus>(e);
    if (k != null)
    {
        KlineStatus? rk = null;
        var redisKeyFormat = string.Format(redisKey, k.Symbol);
        if (redis.HasKey(redisKeyFormat))
        {
            rk = JsonSerializer.Deserialize<KlineStatus>(redis.GetKey(redisKeyFormat));
        }

        if (k.Final)
        {
            logger.LogTrace($"Binance kline update is final {k.Symbol}, {k.CloseTime}");
            await producer.ProduceAsync(Settings.KafkaTopicDbWriter, nameof(KlineStatus), e);
        }

        if(rk != null)
        {
            if(!rk.Final || k.OpenTime >= rk.OpenTime)
            {
                await redis.SetKeyAsync(redisKeyFormat, JsonSerializer.Serialize(k), TimeSpan.FromSeconds(10));
            }
        } else
            await redis.SetKeyAsync(redisKeyFormat, JsonSerializer.Serialize(k), TimeSpan.FromSeconds(10));

        await producer.ProduceAsync(Settings.KafkaTopicRealTime, nameof(KlineStatus), e);
    }
}