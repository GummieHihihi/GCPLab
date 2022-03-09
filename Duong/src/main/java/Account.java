package com.mypackage.pipeline;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@VisibleForTesting
/**
 * A class used for parsing JSON web server events
 */
@DefaultSchema(JavaFieldSchema.class)
public class Account {
    String user_id;
//    @javax.annotation.Nullable Double lat;
//    @javax.annotation.Nullable Double lng;
    String firstName;
    String lastName;
    String street;
}
