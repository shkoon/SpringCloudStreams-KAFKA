package ma.enset.demospringcloudstreamskafka.entities;

import lombok.*;

import java.util.Date;

@Data@AllArgsConstructor@NoArgsConstructor@ToString@Builder
public class PageEvent {
    private String name;
    private String user;
    private Date vistedAt;
    private long duration;
}
