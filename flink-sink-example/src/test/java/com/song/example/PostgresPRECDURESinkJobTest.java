package com.song.example;

import org.junit.Test;

public class PostgresPRECDURESinkJobTest {
    @Test
	public void LoadTest() {

        try {

            String[] args = {};
            PostgresPRECDURESinkJob.main(args);
            
        } catch (Exception e) {

            e.printStackTrace();
            
            String msg = e.getMessage();
            System.out.println(msg);;
        }
	}
}
