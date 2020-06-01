package stream.cases.pv.dto;

import lombok.*;

/**
 * @author SeawayLee
 * @create 2020-05-26 15:35
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class UserVisitWebEvent {
    private String id;
    private String date;
    private Integer pageId;
    private String userId;
    private String url;
}
