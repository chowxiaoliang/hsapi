package core;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class TestG {

    public static void main(String[] args) {
        JsonArray jsonElements = new JsonArray();
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name", "zhouliang");
        jsonObject.addProperty("age", 20);
        jsonElements.add(jsonObject);

        JsonObject jsonObject1 = new JsonObject();
        jsonObject1.addProperty("name", "xiaoliang");
        jsonObject1.addProperty("age", 19);
        jsonElements.add(jsonObject1);

        JsonObject jsonObject2 = new JsonObject();
        jsonObject2.addProperty("name","liang100");
        jsonObject2.addProperty("age", 18);
        jsonElements.add(jsonObject2);

        System.out.println(jsonElements.toString());
    }
}
