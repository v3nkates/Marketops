package marketops.catalog.repository;

import io.javalin.Javalin;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Market Ops Governance System
 * Integrated: DataSource, MarketAsset, DataSet, Model, ETL, and Lineage Registries.
 */
public class marketops {

    private static final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    // --- 1. DATA INFRASTRUCTURE MODELS ---

    public static class DataSource {
        public String id;
        public String name;
        public String type; // live, static
        public String format; // api, json, csv, text
        public String connectionData; // filepath, apiurl
    }

    public static class MarketAsset {
        public String id;
        public String name;
        public String type; // stock, bond, commodity, job-skill, other
        
        // Added per command
        public Double demand;
        public Double currentPrice;
        public List<MarketAssetHistory> history = new ArrayList<>();
    }

    // Helper for MarketAsset History tracking
    public static class MarketAssetHistory {
        public Double price;
        public Double demand;
        public long timestamp = System.currentTimeMillis();

        public MarketAssetHistory(Double price, Double demand) {
            this.price = price;
            this.demand = demand;
        }
    }

    public static class ETL {
        public String id;
        public String name;
        public String triggerType; // scheduled, event-based
        public String dataSourceId;
        public String dataSetId;
        public String path;
        public String language; // python, java, sql
    }

    public static class LineageTracking {
        public String id;
        public String dataSourceId;
        public String marketAssetId;
        public String userId;
        public String modelRegistryId;
        public long timestamp = System.currentTimeMillis();
        public String changeType; // creation, transformation, consumption
    }

    public static class LivePriceTracking {
        public String id;
        public String name;
        public String modelId;
        public String dataSourceId;
        public String simulationTrackingId;
        public Double toleranceRangeStart;
        public Double toleranceRangeEnd;
    }

    public static class DataSet {
        public String id;
        public String name;
        public String description;
        public String schema;
        public String path;
    }

    // --- 2. EXISTING GOVERNANCE MODELS ---

    public static class User {
        public String username;
        public Set<String> permissions = new HashSet<>();
        public User(String username) { this.username = username; }
    }

    public static class ModelRegistry {
        public String id;
        public String modelName;
        public String description;
        public ModelParameterTracking parameterTracking;
    }

    public static class ModelParameterTracking {
        public String id;
        public String registryId;
        public String algorithm;
        public Map<String, Object> parameters;
        public long timestamp = System.currentTimeMillis();
    }

    // ... [DistributionRegistry, SimulationTracking, SimulationData remain as defined] ...

    // --- 3. GOVERNANCE ENGINE ---

    public static class GovernanceEngine {
        private final Map<String, User> userRegistry = new ConcurrentHashMap<>();
        private final Map<String, Object> objectStore = new ConcurrentHashMap<>();

        public void addUser(String name) { userRegistry.put(name.toLowerCase(), new User(name)); }
        public void registerObject(String id, Object obj) { objectStore.put(id.toLowerCase(), obj); }

        public String executeGrant(String command) {
            try {
                String[] parts = command.toUpperCase().split("\\s+");
                if (parts.length >= 8 && parts[0].equals("GRANT")) {
                    String action = parts[1];
                    String objectId = parts[4].toLowerCase();
                    String username = parts[7].toLowerCase();

                    if (userRegistry.containsKey(username)) {
                        userRegistry.get(username).permissions.add(action + ":" + objectId);
                        return "SUCCESS: Granted " + action + " on " + objectId + " to " + username;
                    }
                }
            } catch (Exception e) { return "ERROR: Invalid syntax."; }
            return "ERROR: Failure to process grant.";
        }

        public boolean hasAccess(String user, String action, String obj) {
            User u = userRegistry.get(user.toLowerCase());
            return u != null && u.permissions.contains(action.toUpperCase() + ":" + obj.toLowerCase());
        }

        public Object getObject(String id) { return objectStore.get(id.toLowerCase()); }
    }

    public static void main1(String[] args) {
        // Initialization and API logic remains similar
        System.out.println("\n>>> Market Ops Unified Catalog Online with ETL & Lineage Support.");
    }
}