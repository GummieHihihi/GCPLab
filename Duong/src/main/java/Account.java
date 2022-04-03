import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.data.Json;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import java.io.Serializable;

@VisibleForTesting
/**
 * A class used for parsing JSON web server events
 */
@DefaultSchema(JavaFieldSchema.class)
public class Account implements Serializable{
    private int userId;
//    @javax.annotation.Nullable Double lat;
//    @javax.annotation.Nullable Double lng;
    private String fullName;
    private String surName;

    public int getUserId() {
        return userId;
    }

    public String getFullName() {
        return fullName;
    }

    public String getSurName() {
        return surName;
    }

    @Override
    public String toString() {
        return "Account{" +
                "userId='" + userId + '\'' +
                ", surName='" + surName;
    }
}
