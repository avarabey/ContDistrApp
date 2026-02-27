package com.contdistrapp.refdata.service;

import com.contdistrapp.refdata.domain.UpdateCommand;

public interface CommandPublisher {

    void publish(UpdateCommand command);
}
