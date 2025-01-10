package kh.edu.istad.elastic_service.service;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kh.edu.istad.elastic_service.domain.CommunityEngagement;
import kh.edu.istad.elastic_service.domain.Content;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticService {

    private final ElasticsearchOperations elasticsearchOperations;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "content-service.contents", groupId = "content-group")
    public void handleContentChange(String message, Acknowledgment acknowledgment) {
        try {
            // Log the raw message for debugging
            log.info("Received Kafka message: {}", message);

            // Clean the message by removing invalid characters
            String cleanedMessage = message.replaceAll("[^\\x20-\\x7E]", "");

            // Log the cleaned message for debugging
            log.info("Cleaned Kafka message: {}", cleanedMessage);

            // Deserialize the cleaned Kafka message into a Map
            Map<String, Object> messageMap = objectMapper.readValue(cleanedMessage, Map.class);

            // Extract the operationType from the message
            String operationType = (String) messageMap.get("operationType");

            // Handle the operation based on the operationType
            switch (operationType) {
                case "insert":
                    handleInsertOperation(messageMap);
                    break;
                case "update":
                    handleUpdateOperation(messageMap);
                    break;
                case "delete":
                    handleDeleteOperation(messageMap);
                    break;
                default:
                    log.warn("Unsupported operationType: {}", operationType);
            }

            // Manually acknowledge the message
            acknowledgment.acknowledge();
        } catch (JsonParseException e) {
            log.error("Malformed JSON message: {}", message, e);
            // Optionally, you can acknowledge the message to skip it
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Error processing Kafka message: {}", e.getMessage(), e);
        }
    }

    private void handleInsertOperation(Map<String, Object> messageMap) {
        // Extract the fullDocument part from the message
        Map<String, Object> fullDocument = (Map<String, Object>) messageMap.get("fullDocument");

        // Map the MongoDB document to the Elasticsearch Content model
        Content content = mapToContent(fullDocument);

        // Save the content to Elasticsearch
        elasticsearchOperations.save(content);
        log.info("Content inserted into Elasticsearch: {}", content.getId());
    }

    private void handleUpdateOperation(Map<String, Object> messageMap) {
        // Extract the fullDocument part from the message
        Map<String, Object> fullDocument = (Map<String, Object>) messageMap.get("fullDocument");

        // Map the MongoDB document to the Elasticsearch Content model
        Content content = mapToContent(fullDocument);

        // Update the content in Elasticsearch
        elasticsearchOperations.save(content);
        log.info("Content updated in Elasticsearch: {}", content.getId());
    }

    private void handleDeleteOperation(Map<String, Object> messageMap) {
        // Extract the documentKey part from the message
        Map<String, Object> documentKey = (Map<String, Object>) messageMap.get("documentKey");
        String id = ((Map<String, String>) documentKey.get("_id")).get("$oid");

        // Delete the content from Elasticsearch
        elasticsearchOperations.delete(id, Content.class);
        log.info("Content deleted from Elasticsearch: {}", id);
    }

    private Content mapToContent(Map<String, Object> fullDocument) {
        Content content = new Content();
        content.setId(fullDocument.get("_id").toString());
        content.setTitle((String) fullDocument.get("title"));
        content.setContent((String) fullDocument.get("content"));
        content.setThumbnail((String) fullDocument.get("thumbnail"));
        content.setSlug((String) fullDocument.get("slug"));
        content.setTags((List<String>) fullDocument.get("tags"));

        // Map communityEngagement
        Map<String, Object> engagementMap = (Map<String, Object>) fullDocument.get("communityEngagement");
        CommunityEngagement engagement = new CommunityEngagement();
        engagement.setLikeCount(parseNumber(engagementMap.get("likeCount")));
        engagement.setCommentCount(parseNumber(engagementMap.get("commentCount")));
        engagement.setReportCount(parseNumber(engagementMap.get("reportCount")));
        engagement.setFireCount(parseNumber(engagementMap.get("fireCount")));
        engagement.setLoveCount(parseNumber(engagementMap.get("loveCount")));
        engagement.setLastUpdated(parseDate(engagementMap.get("lastUpdated")));
        content.setCommunityEngagement(engagement);

        content.setIsDeleted((Boolean) fullDocument.get("isDeleted"));
        content.setIsDraft((Boolean) fullDocument.get("isDraft"));
        content.setAuthorUuid((String) fullDocument.get("author_uuid"));

        // Map dates
        content.setCreatedDate(parseDate(fullDocument.get("created_date")));
        content.setLastModifiedDate(parseDate(fullDocument.get("last_modified_date")));

        return content;
    }

    private Long parseNumber(Object numberObject) {
        if (numberObject instanceof Map) {
            // Handle MongoDB's $numberLong format
            Map<String, String> numberMap = (Map<String, String>) numberObject;
            return Long.parseLong(numberMap.get("$numberLong"));
        } else if (numberObject instanceof Number) {
            // Handle direct numeric values
            return ((Number) numberObject).longValue();
        }
        return 0L; // Default value if parsing fails
    }

    private LocalDateTime parseDate(Object dateObject) {
        if (dateObject instanceof Map) {
            // Handle MongoDB's $date format
            Map<String, Object> dateMap = (Map<String, Object>) dateObject;
            Object dateValue = dateMap.get("$date");

            if (dateValue instanceof Long) {
                // Handle timestamp in milliseconds
                long timestamp = (Long) dateValue;
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            } else if (dateValue instanceof String) {
                // Handle ISO date string
                String dateString = (String) dateValue;
                return LocalDateTime.parse(dateString, DateTimeFormatter.ISO_DATE_TIME);
            }
        } else if (dateObject instanceof String) {
            // Handle direct date strings
            return LocalDateTime.parse((String) dateObject, DateTimeFormatter.ISO_DATE_TIME);
        } else if (dateObject instanceof Long) {
            // Handle direct timestamp in milliseconds
            long timestamp = (Long) dateObject;
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        }
        return null; // Default value if parsing fails
    }
}