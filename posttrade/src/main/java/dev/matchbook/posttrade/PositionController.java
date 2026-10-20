package dev.matchbook.posttrade;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** Read-only position and P&L reports, in currency units, from the {@code position_report} view. */
@RestController
public class PositionController {

    private final JdbcTemplate jdbc;

    public PositionController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/api/v1/positions/{clientId}")
    public List<Map<String, Object>> positions(@PathVariable long clientId) {
        return jdbc.queryForList("select * from position_report where client_id = ? order by symbol", clientId);
    }
}
