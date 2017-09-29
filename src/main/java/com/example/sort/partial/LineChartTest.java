package com.example.sort.partial;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.example.util.Opener;

import be.ceau.chart.LineChart;
import be.ceau.chart.dataset.LineDataset;

public class LineChartTest {

	/*
	 * 미션! 맵파일하고 연동해서
	 * 라벨에 입력된 값을 기준으로 튜플 수 구해서 라인차트 그리
	 */
	
	
	@Test
	public void test() throws IOException {

		LineChart line = new LineChart();
		line.setData(LineChart.data());
		line.setOptions(LineChart.options());
		
		line.getData().setLabels("100마일", "200마일", "300마일", "400마일", "500마일");
	
		LineDataset set1 = new LineDataset();
	
		set1.setData(3350, 2450, 3000, 5040, 4000);		//실제 데이터
		set1.setLabel("1988");
		
		line.getData().addDataset(set1);
		
		Opener.inBrowser(line);
	}

}
