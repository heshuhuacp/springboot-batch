package com.example.batch.process;

import com.example.batch.domain.WorkMan;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.batch.item.validator.ValidationException;

public class CsvItemProcessor extends ValidatingItemProcessor<WorkMan>{

    @Override
    public WorkMan process(WorkMan item) throws ValidationException {

        super.process(item);

        if(item.getNation().equals("汉族")){
            item.setNation("01");
        }else{
            item.setNation("02");
        }
        return item;
    }
}
