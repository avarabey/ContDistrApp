package com.contdistrapp.refdata.domain;

public record ApplyOutcome(boolean committed, Long committedVersion, String failureMessage) {

    public static ApplyOutcome pending() {
        return new ApplyOutcome(false, null, null);
    }

    public static ApplyOutcome committed(long version) {
        return new ApplyOutcome(true, version, null);
    }

    public static ApplyOutcome failed(String message) {
        return new ApplyOutcome(false, null, message);
    }
}
