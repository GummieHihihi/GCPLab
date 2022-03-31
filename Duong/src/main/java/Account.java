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
    public int userId;
//    @javax.annotation.Nullable Double lat;
//    @javax.annotation.Nullable Double lng;
    public String firstName;
    public String lastName;
    public String street;

    @Override
    public String toString() {
        return "Account{" +
                "userId='" + userId + '\'' +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", street='" + street + '\'' +
                '}';
    }
}
