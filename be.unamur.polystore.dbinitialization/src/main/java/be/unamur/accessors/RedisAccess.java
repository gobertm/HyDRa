package be.unamur.accessors;

import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class LuaRedis {
    public static void main(String args[]) {
        Jedis jedis = new Jedis("localhost", 6363);

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

        System.out.println();
    }
}
