package com.mingoburton.chatgptauto;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;

public class MainActivity extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        // Never use a web URL here.
        // Route the phone launcher directly through the same bridge used by Android Auto.
        Intent bridge = new Intent(this, VoiceBridgeActivity.class);
        bridge.addFlags(
                Intent.FLAG_ACTIVITY_NEW_TASK |
                Intent.FLAG_ACTIVITY_CLEAR_TOP
        );
        startActivity(bridge);
        finish();
    }
}
