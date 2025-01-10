package kh.edu.istad.elastic_service.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.LastModifiedDate;

import java.time.LocalDateTime;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CommunityEngagement {
//    likeCount, commentCount, shareCount, reportCount, and lastUpdated
    private Long likeCount;

    private Long commentCount;

    private Long reportCount;

    private Long fireCount;

    private Long loveCount;

    private LocalDateTime lastUpdated;
}
