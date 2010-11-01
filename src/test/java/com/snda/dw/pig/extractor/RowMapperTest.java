package com.snda.dw.pig.extractor;

import java.io.IOException;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import com.snda.dw.pig.util.Bags;

public class RowMapperTest extends TestCase {

    private static class Page {
        private String url;
        private double page_rank;
        private long impressions;
    }

    private static class PageRowMapper implements RowMapper<Page> {
        @Override
        public Page map(Tuple tuple) throws IOException {
            Page page = new Page();
            page.url = tuple.get(0).toString();
            page.page_rank = (Double) tuple.get(1);
            page.impressions = (Long) tuple.get(2);
            return page;
        }
    }

    public void testRowMapper() throws IOException {
        PageRowMapper mapper = new PageRowMapper();
        DataBag bag = Bags.newBag(3, "http://google.com", 1.0, 100L,
                "http://yahoo.com", 0.9, 120L);
        Iterator<Tuple> iter = bag.iterator();
        Page page = mapper.map(iter.next());
        assertEquals("http://google.com", page.url);
        assertEquals(1.0, page.page_rank, 1E-6);
        assertEquals(100L, page.impressions);

        page = mapper.map(iter.next());
        assertEquals("http://yahoo.com", page.url);
        assertEquals(0.9, page.page_rank, 1E-6);
        assertEquals(120L, page.impressions);
    }
}
