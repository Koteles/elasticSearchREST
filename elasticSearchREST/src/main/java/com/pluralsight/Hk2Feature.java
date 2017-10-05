package com.pluralsight;

import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.ext.Provider;

@Provider
public class Hk2Feature implements Feature {

    @Override
    public boolean configure(FeatureContext context) {
        context.register(new MyApplicationBinder());
        return true;
    }
}
