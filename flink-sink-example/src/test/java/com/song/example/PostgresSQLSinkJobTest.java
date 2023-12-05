package com.song.example;

import org.junit.Test;

public class PostgresSQLSinkJobTest {

    @Test
	public void LoadTest() {

        try {

            String[] args = {};
            PostgresSQLSinkJob.main(args);
            
        } catch (Exception e) {

            e.printStackTrace();
            
            String msg = e.getMessage();
            System.out.println(msg);;
        }
	}

}