/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.config;

import java.util.Map;
import java.util.Optional;

public interface PropertiesSource {

    Map<String, String> values();

    Optional<String> getValue(String key);

}
