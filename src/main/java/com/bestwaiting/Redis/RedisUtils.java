package com.bestwaiting.Redis;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtils {
	private static Logger logger=Logger.getLogger(RedisUtils.class);
	private static String REDIS_IP;
    private static int REDIS_PORT;
    private static String REDIS_PASSWORD;
    private static JedisPool redisPool;
    static{
    	Config conf=Config.getInstance();
    	REDIS_IP=conf.getString("jedis.ip","101.200.190.129");
    	REDIS_PORT=conf.getInt("jedis.port",6379);
    	REDIS_PASSWORD=conf.getString("jedis.password",null);
        JedisPoolConfig config=new JedisPoolConfig();
        config.setMaxActive(conf.getInt("redis.pool.maxActive", 5000));
        config.setMaxIdle(conf.getInt("redis.pool.maxIdle", 5000));
        config.setMaxWait(conf.getLong("redis.pool.maxWait", 5000L));
        config.setTestOnBorrow(conf.getBoolean("redis.pool.testOnBorrow", true));
        config.setTestOnReturn(conf.getBoolean("redis.pool.setTestOnReturn", true));
        config.setTestWhileIdle(conf.getBoolean("redis.pool.setTestWhileIdle", true));
        redisPool=new JedisPool(config,REDIS_IP,REDIS_PORT,60000);
    }
    /**
     * 获取数据
     * @param key
     * @return string/byte
     */
    public static String get(String key){
        String value=null;
        Jedis jedis=null;
        try{
            jedis=redisPool.getResource();
            value=jedis.get(key);
        }catch (Exception e){
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        }finally {
            close(jedis);
        }
        return value;
    }
    public static byte[] get(byte[] key){
        byte[] value = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            value = jedis.get(key);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }

        return value;
    }
    
    /**
     * 设置数据
     * @param key
     * @param value
     */
    public static void set(byte[] key, byte[] value) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.set(key, value);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
    public static void set(byte[] key, byte[] value, int time) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.set(key, value);
            jedis.expire(key, time);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
    public static void set(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.set(key, value);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
    public static void set(String key, String value, int time) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.set(key, value);
            jedis.expire(key, time);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
    /**
     * 删除已存在的键。不存在的 key 会被忽略。
     * @param key
     */
    public static void del(String key) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.del(key);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
    
    /**
	 * 将哈希表 key 中的字段 field 的值设为 value 。
	 * @param key
	 * @param field
	 * @param value
	 */
    public static void hset(byte[] key, byte[] field, byte[] value) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.hset(key, field, value);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
    public static void hset(String key, String field, String value) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.hset(key, field, value);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }

    /**
     * 用于返回哈希表中指定字段的值。
     * @param key
     * @return
     */
    public static byte[] hget(byte[] key, byte[] field) {
        byte[] value = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            value = jedis.hget(key, field);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
        return value;
    }
    public static String hget(String key, String field) {
        String value = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            value = jedis.hget(key, field);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
        return value;
    }
    /**
     * 用于删除哈希表 key 中的一个或多个指定字段，不存在的字段将被忽略。
     * @param key
     * @param field
     */
    public static void hdel(byte[] key, byte[] field) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.hdel(key, field);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
    public static void hdel(String key, String... field) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.hdel(key, field);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
   
    /**
     * 移除队列队尾数据
     * @param  key 键名
     * @return
     */
    public static byte[] rpop(byte[] key) {
        byte[] bytes = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            bytes = jedis.rpop(key);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
        return bytes;
    }
    public static String rpop(String key) {
    	String str = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            str = jedis.rpop(key);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
        return str;
    }
    /**
     * 用于同时将多个 field-value (字段-值)对设置到哈希表中。此命令会覆盖哈希表中已存在的字段。如果哈希表不存在，会创建一个空哈希表，并执行 HMSET 操作。
     * @param key
     * @param hash
     */
    public static void hmset(Object key, Map hash) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.hmset(key.toString(), hash);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);

        }
    }
    public static void hmset(Object key, Map hash, int time) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.hmset(key.toString(), hash);
            jedis.expire(key.toString(), time);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());

        } finally {
            close(jedis);

        }
    }
    /**
     * 用于返回哈希表中，一个或多个给定字段的值。如果指定的字段不存在于哈希表，那么返回一个 nil 值。
     * @param key
     * @param fields
     * @return
     */
    public static List hmget(Object key, String... fields) {
        List result = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            result = jedis.hmget(key.toString(), fields);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());

        } finally {
            close(jedis);

        }
        return result;
    }
    /**
     * 获取哈希表中的所有字段名。
     * @param key
     * @return
     */
    public static Set hkeys(String key) {
        Set result = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            result = jedis.hkeys(key);
        } catch (Exception e) {
            //释放redis对象
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());

        } finally {
            //返还到连接池
            close(jedis);

        }
        return result;
    }
    public static Set hkeys(byte[] key) {
        Set result = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            result = jedis.hkeys(key);
        } catch (Exception e) {
            //释放redis对象
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());

        } finally {
            //返还到连接池
            close(jedis);

        }
        return result;
    }
   
    /**
     * 返回哈希表中，所有的字段和值。
     * @param key
     * @return
     */
    public static Map hgetAll(String key) {
        Map result = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            result = jedis.hgetAll(key);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
        return result;
    }
    public static Map hgetAll(byte[] key) {
        Map result = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            result = jedis.hgetAll(key);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
        return result;
    }
    
    
    /**
     * 存储REDIS队列 顺序存储
     * @param  key reids键名
     * @param  value 键值
     */
    public static void lpush(byte[] key, byte[] value) {

        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.lpush(key, value);
        } catch (Exception e) {
            //释放redis对象
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            //返还到连接池
            close(jedis);
        }
    }
    public static void lpush(String key, String... value) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.lpush(key, value);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
    /**
     * 存储REDIS队列 反向存储
     * @param  key reids键名
     * @param  value 键值
     */
    public static void rpush(byte[] key, byte[] value) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.rpush(key, value);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
    public static void rpush(String key, String... value) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.rpush(key, value);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
    /**
     * 用于移除列表的最后一个元素，并将该元素添加到另一个列表并返回。
     * @param  key reids键名
     * @param  destination 键值
     */
    public static void rpoplpush(byte[] key, byte[] destination) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.rpoplpush(key, destination);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
    public static String rpoplpush(String key, String destination) {
    	String str=null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            str=jedis.rpoplpush(key, destination);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
        return str;
    }
    /**
     * 移除队列队首数据
     * @param  key 键名
     * @return
     */
    public static byte[] lpop(byte[] key) {
        byte[] bytes = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            bytes = jedis.lpop(key);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
        return bytes;
    }
    public static String lpop(String key) {
    	String str = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            str = jedis.lpop(key);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
        return str;
    }
     /**
     * 返回列表中指定区间内的元素； 其中 0 表示列表的第一个元素， 1 表示列表的第二个元素，以此类推。 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
     * @param key
     * @param from 开始
     * @param to   结束
     * @return
     */
    public static List lrange(String key, int from, int to) {
        List result = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            result = jedis.lrange(key, from, to);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());

        } finally {
            close(jedis);

        }
        return result;
    }
    public static List lrange(byte[] key, int from, int to) {
        List result = null;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            result = jedis.lrange(key, from, to);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());

        } finally {
            close(jedis);

        }
        return result;
    }
    
    
    public static void del(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.del(key);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
    }
    /**
     * 用于返回列表的长度。 如果列表 key 不存在，则 key 被解释为一个空列表，返回 0 。 如果 key 不是列表类型，返回一个错误。
     * @param key
     * @return 列表的长度
     */
    public static long llen(String key) {
        long len = 0;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.llen(key);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
        return len;
    }
    public static long llen(byte[] key) {
        long len = 0;
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.llen(key);
        } catch (Exception e) {
            redisPool.returnBrokenResource(jedis);
            logger.error(e.getMessage());
        } finally {
            close(jedis);
        }
        return len;
    }
    
    private static void close(Jedis jedis) {
        try{
            redisPool.returnResource(jedis);
        }catch (Exception e){
            if(jedis.isConnected()){
                jedis.quit();
                jedis.disconnect();
            }
        }
    }
}