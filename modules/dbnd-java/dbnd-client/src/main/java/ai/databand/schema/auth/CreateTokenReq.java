package ai.databand.schema.auth;

import java.util.UUID;

public class CreateTokenReq {

    private final String label;
    private final String lifespan;

    public CreateTokenReq() {
        this(UUID.randomUUID().toString(), "3600");
    }

    public CreateTokenReq(String label, String lifespan) {
        this.label = label;
        this.lifespan = lifespan;
    }

    public String getLabel() {
        return label;
    }

    public String getLifespan() {
        return lifespan;
    }
}
