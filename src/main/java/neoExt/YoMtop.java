package neoExt;

/**
 * Created by lyonwj on 2/8/14.
 */

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Iterator;
import java.util.ArrayList;

import com.google.gson.Gson;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.neo4j.graphdb.GraphDatabaseService;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;


@Path("/recommend")
public class YoMtop {

    private final ExecutionEngine executionEngine;

    public YoMtop( @Context GraphDatabaseService database)
    {
        this.executionEngine = new ExecutionEngine(database);
    }

    @GET
    //@Produces(MediaType.TEXT_PLAIN)
    @Path("/{user1}/{user2}/{user3}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRecommendationForSpecifiedUsers(@PathParam("user1") String user1, @PathParam("user2") String user2, @PathParam("user3") String user3){
        Map<String, Object> params = new HashMap<String, Object>();
        Map<String, Object> jsonMap = new HashMap<String, Object>();

        ArrayList<Object> rests = new ArrayList<Object>();
        ArrayList<Object> users = new ArrayList<Object>();

        params.put("user1", user1);
        params.put("user2", user2);
        params.put("user3", user3);
        params.put("thres", 3);

        String query =  "MATCH (u1:User{id:{user1}}) \n " +
                        "MATCH (u2:User{id: {user2}}) \n " +
                        "MATCH (u3:User{id: {user3}}) \n " +
                "MATCH path2=(rest)-[:REVIEW_OF]-(r7:Review)-[:WRITTEN_BY]-()-[:WRITTEN_BY]-(r8:Review)-[:REVIEW_OF]-()-[:REVIEW_OF]-(r9:Review)-[:WRITTEN_BY]-(u3) " +
                "WHERE r7.stars > {thres} AND r8.stars > {thres} AND r9.stars > {thres} WITH rest, u3, u2, u1, r7.stars+r8.stars+r9.stars AS scores \n" +
                "MATCH path=(u1)-[:WRITTEN_BY]-(r1:Review)-[:REVIEW_OF]-()-[:REVIEW_OF]-(r2:Review)-[:WRITTEN_BY]-()-[:WRITTEN_BY]-(r3:Review)-[:REVIEW_OF]-(rest)-[:REVIEW_OF]-(r4:Review)-[:WRITTEN_BY]-()-[:WRITTEN_BY]-(r5:Review)-[:REVIEW_OF]-()<-[:REVIEW_OF]-(r6:Review)-[:WRITTEN_BY]-(u2) "  +
                "WHERE r1.stars > {thres} AND r2.stars > {thres} AND r3.stars > {thres} AND r4.stars > {thres} AND r5.stars > {thres} AND r6.stars > {thres} " +
                "RETURN DISTINCT scores+r1.stars+r2.stars+r3.stars+r4.stars+r5.stars+r6.stars AS score, rest.name as name, rest.stars as stars, rest.address as address, u1.id as user1, u2.id as user2, u3.id as user3 LIMIT 2";

        Iterator<Map<String, Object>> result = executionEngine.execute(query, params).iterator();



        while (result.hasNext()){
            Map<String,Object> row = result.next();
            Map<String, Object> restMap = new HashMap<String, Object>();

            //jsonMap.put("recommendation", row.get("name"));
            //jsonMap.put("score", row.get("score"));

            restMap.put("name", row.get("name"));
            restMap.put("address", row.get("address"));
            restMap.put("stars", row.get("stars"));
            restMap.put("score", row.get("score"));
            rests.add(restMap);




        }
        users.add(user1);
        users.add(user2);
        users.add(user3);
        jsonMap.put("users", users);
        jsonMap.put("recommendations", rests);
        jsonMap.put("threshold", 3);

        Gson gson = new Gson();
        String json = gson.toJson(jsonMap);

        return Response.ok(json, MediaType.APPLICATION_JSON).build();

    }


    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/random")
    public Response getRecommendationForRandomUsers(){
        Random rand = new Random();

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int max = 40000;
        int min = 1;
        int random1 = rand.nextInt((max - min) + 1) + min;
        int random2 = rand.nextInt((max-min)+1)+min;
        int random3 = rand.nextInt((max-min)+1)+min;

        Map<String, Object> params = new HashMap<String, Object>();

        params.put("random1", random1);
        params.put("random2", random2);
        params.put("random3", random3);
        params.put("thres", 3);

        String query =  "MATCH (u1:User) WITH u1 SKIP {random1} LIMIT 1 \n" + //"MATCH (u1:User{id:'K_4Eulxwh9fPH-BwqTBSBw'}), " +
                "MATCH (u2:User) WITH u1, u2 SKIP {random2} LIMIT 1 \n" + //"(u2:User{id: 'ANTrWPSoOqbXkvLvxes-aA'}), " +
                "MATCH (u3:User) WITH u1, u2, u3 SKIP {random3} LIMIT 1 \n" + //"(u3:User{id:'spNqsxYL6xDMR_UbvtTJ_Q'}), " +
                //"RETURN u1.name as name , u2.name as name2, u3.name as name3";

                "MATCH path2=(rest)-[:REVIEW_OF]-(r7:Review)-[:WRITTEN_BY]-()-[:WRITTEN_BY]-(r8:Review)-[:REVIEW_OF]-()-[:REVIEW_OF]-(r9:Review)-[:WRITTEN_BY]-(u3) " +
                "WHERE r7.stars > {thres} AND r8.stars > {thres} AND r9.stars > {thres} WITH rest, u3, u2, u1, r7.stars+r8.stars+r9.stars AS scores \n" +
                "MATCH path=(u1)-[:WRITTEN_BY]-(r1:Review)-[:REVIEW_OF]-()-[:REVIEW_OF]-(r2:Review)-[:WRITTEN_BY]-()-[:WRITTEN_BY]-(r3:Review)-[:REVIEW_OF]-(rest)-[:REVIEW_OF]-(r4:Review)-[:WRITTEN_BY]-()-[:WRITTEN_BY]-(r5:Review)-[:REVIEW_OF]-()<-[:REVIEW_OF]-(r6:Review)-[:WRITTEN_BY]-(u2) "  +
                "WHERE r1.stars > {thres} AND r2.stars > {thres} AND r3.stars > {thres} AND r4.stars > {thres} AND r5.stars > {thres} AND r6.stars > {thres} " +
                "RETURN scores+r1.stars+r2.stars+r3.stars+r4.stars+r5.stars+r6.stars AS score, rest.name as name, u1.id as user1, u2.id as user2, u3.id as user3 LIMIT 1";
        //"MATCH path2=(rest)<-[:REVIEW_OF]-(r7:Review)-[:WRITTEN_BY]->()<-[:WRITTEN_BY]-(r8:Review)-[:REVIEW_OF]->()<-[:REVIEW_OF]-(r9:Review)-[:WRITTEN_BY]->(u3) WHERE r1.stars > 2 AND r2.stars > 2 AND r3.stars > 2 AND r4.stars > 2 AND r5.stars > 2 AND r6.stars > 2 AND r7.stars > 2 AND r8.stars > 2 AND r9.stars > 2 RETURN rest.name as name, (r1.stars+r2.stars+r3.stars+r4.stars+r5.stars+r6.stars+r7.stars+r8.stars+r9.stars)/9 as score LIMIT 1";

        //return engine.execute(query, params).iterator();

        ExecutionResult result = executionEngine.execute(query, params);
        String rest = String.valueOf(result.columnAs("name").next());


        return Response.status(Status.OK).entity(
                ("Recommendation: " + rest).getBytes(Charset.forName("UTF-8"))).build();

    }

}
