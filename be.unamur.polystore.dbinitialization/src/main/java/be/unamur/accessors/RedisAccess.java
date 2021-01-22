package be.unamur.accessors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class RedisAccess {
    private String host;
    private int port;
    private Jedis jedis;

    public RedisAccess(String host, int port) {
        this.host = host;
        this.port = port;
        jedis = new Jedis(host, port);
    }

    public void redisLua(){
        //register lua script
//        InputStream luaInputStream = LuaRedis.class.getClassLoader().getResourceAsStream("luaScript.lua");
//        String luaScript = new BufferedReader(new InputStreamReader(luaInputStream))
//                .lines()
//                .collect(Collectors.joining("\n"));
        String luaSHA = jedis.scriptLoad("return redis.call('set','foo','bar')");


        //Eval lua Script
        List<String> KEYS = Collections.singletonList("PROFESSOR:");
        List<String> ARGS = Collections.singletonList("{}");
        jedis.evalsha(luaSHA, KEYS, ARGS);
    }

    public static void main(String args[]) {
        RedisAccess redis = new RedisAccess("localhost", 6363);
//        redis.redisLua();
        redis.command();
    }

    private void command() {
        String cursor="0";
        ScanResult scanResult;
        ScanParams scanParams = new ScanParams().match("PROFESSOR:*:NAME");
        boolean cursorFinished = false;
        while(!cursorFinished){
            scanResult = jedis.scan(cursor,scanParams);
            cursor = scanResult.getCursor();
            scanResult.getResult().forEach(System.out::println);
            if(cursor.equals("0"))
                cursorFinished=true;
        }
    }
}
