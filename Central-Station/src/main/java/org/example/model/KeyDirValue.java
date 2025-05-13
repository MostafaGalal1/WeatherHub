package org.example.model;

import java.io.File;

public record KeyDirValue(
        File fileId,
        Long valuePosition,
        Integer valueSize,
        Long timeStamp
) {
}
