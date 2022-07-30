/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema.auth;


import java.util.Optional;

public class LoginReq {

    private final String username;
    private final String password;

    public LoginReq() {
        this.username = Optional.ofNullable(System.getenv("DBND__TRACKER__USERNAME")).orElse("databand");
        this.password = Optional.ofNullable(System.getenv("DBND__TRACKER__PASSWORD")).orElse("databand");
    }

    public LoginReq(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
