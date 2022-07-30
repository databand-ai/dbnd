/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.id;

public class Sha1Short {

    private final String value;

    public Sha1Short(String namespace, String name) {
        this.value = new Sha1Long(namespace, name).toString().substring(0, 8);
    }

    @Override
    public String toString() {
        return value;
    }

}
