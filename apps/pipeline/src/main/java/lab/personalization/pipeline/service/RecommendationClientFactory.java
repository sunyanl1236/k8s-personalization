package lab.personalization.pipeline.service;

import java.io.Serializable;

// The factory is what travels to the TaskManagers, not the client: a client owns
// an executor and is not serializable.
public interface RecommendationClientFactory extends Serializable {

    RecommendationClient create();
}
