package Publisher;

import com.github.javafaker.Faker;
import org.json.simple.JSONObject;

import java.nio.charset.Charset;
import java.util.Random;

public class GenerateData {

    Faker faker = new Faker();

    public String createRightMessage(int personId){

        String name = faker.name().fullName();
        String firstName = faker.name().firstName();
        String lastName = faker.name().lastName();
        String streetAddress = faker.address().streetAddress();

        JSONObject json = new JSONObject();

        json.put("ID", personId);
        json.put("fullName", name);
        json.put("firstName", firstName);
        json.put("lastName", lastName);
        json.put("street", streetAddress);

        String message = json.toString();
        return message;
    }
    public String createWrongMessage(){
        Random random = new Random();
        int randomNumber = random.nextInt();
        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);
        String generatedString = new String(array, Charset.forName("UTF-8"));

        return generatedString + randomNumber;
    }
}
