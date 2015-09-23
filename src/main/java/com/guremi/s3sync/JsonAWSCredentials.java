package com.guremi.s3sync;

import com.amazonaws.auth.AWSCredentials;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author htaka
 */
public class JsonAWSCredentials implements AWSCredentials {
    
    
    Config config;
    
    public JsonAWSCredentials() {
        config = ConfigFactory.load("config");
    }

    @Override
    public String getAWSAccessKeyId() {
        return config.getString("account.key");
    }

    @Override
    public String getAWSSecretKey() {
        return config.getString("account.secret");
    }
    
}
