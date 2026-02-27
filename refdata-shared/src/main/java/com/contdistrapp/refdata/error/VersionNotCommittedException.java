package com.contdistrapp.refdata.error;

public class VersionNotCommittedException extends RuntimeException {

    private final long requestedVersion;
    private final long committedVersion;

    public VersionNotCommittedException(long requestedVersion, long committedVersion) {
        super("VERSION_NOT_COMMITTED requested=" + requestedVersion + " committed=" + committedVersion);
        this.requestedVersion = requestedVersion;
        this.committedVersion = committedVersion;
    }

    public long getRequestedVersion() {
        return requestedVersion;
    }

    public long getCommittedVersion() {
        return committedVersion;
    }
}
