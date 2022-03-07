package Publisher;

import com.github.javafaker.Faker;
import org.json.simple.JSONObject;

import java.util.Random;

public class Send_Data {
    Faker faker = new Faker();

    private final String input_topic = "projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-1";

    private String createRightMessage(){

        String name = faker.name().fullName();
        String firstName = faker.name().firstName();
        String lastName = faker.name().lastName();
        String streetAddress = faker.address().streetAddress();

        JSONObject json = new JSONObject();

        json.put("fullName", name);
        json.put("firstName", firstName);
        json.put("lastName", lastName);
        json.put("street", streetAddress);

        String message = json.toString();
        return message;
    }
    private String createWrongMessage(){

    }
    public static void main(String[] args) {

    }
}
