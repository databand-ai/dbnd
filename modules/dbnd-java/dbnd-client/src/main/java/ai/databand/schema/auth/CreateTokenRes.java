/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema.auth;

public class CreateTokenRes {

    private String uid;
    private String token;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}
